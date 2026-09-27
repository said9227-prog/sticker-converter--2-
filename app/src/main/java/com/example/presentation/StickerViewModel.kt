package com.example.presentation

import android.app.Application
import android.net.Uri
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import com.example.conversion.BatchConversionProgress
import com.example.conversion.StickerConversionEngine
import com.example.core.model.ConversionStatus
import com.example.core.model.StickerItem
import com.example.core.model.StickerPack
import com.example.core.model.StickerType
import com.example.core.model.ValidationResult
import com.example.data.db.AppDatabase
import com.example.data.repository.PackRepository
import com.example.data.repository.StickerRepository
import com.example.diagnostics.DiagnosticStage
import com.example.diagnostics.DiagnosticsRepository
import com.example.media.MediaDetector
import com.example.storage.SafeZipExtractor
import com.example.storage.TempFileManager
import com.example.telegram.TelegramStickerValidator
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.io.File
import java.io.FileOutputStream

data class DetectedImportItem(
    val uriString: String,
    val name: String,
    val type: StickerType,
    val sizeBytes: Long,
    val width: Int = 0,
    val height: Int = 0,
    val durationMs: Long = 0L
)

class StickerViewModel(application: Application) : AndroidViewModel(application) {

    private val db = AppDatabase.getInstance(application)
    private val packRepository = PackRepository(db.stickerDao())
    private val stickerRepository = StickerRepository(db.stickerDao())
    val tempFileManager = TempFileManager(application)
    private val conversionEngine = StickerConversionEngine(application, tempFileManager, stickerRepository)
    private val safeZipExtractor = SafeZipExtractor()

    val packs: StateFlow<List<StickerPack>> = packRepository.getAllPacks()
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    private val _currentPack = MutableStateFlow<StickerPack?>(null)
    val currentPack: StateFlow<StickerPack?> = _currentPack.asStateFlow()

    private val _currentStickers = MutableStateFlow<List<StickerItem>>(emptyList())
    val currentStickers: StateFlow<List<StickerItem>> = _currentStickers.asStateFlow()

    private val _conversionProgress = MutableStateFlow<BatchConversionProgress?>(null)
    val conversionProgress: StateFlow<BatchConversionProgress?> = _conversionProgress.asStateFlow()

    private val _isImporting = MutableStateFlow(false)
    val isImporting: StateFlow<Boolean> = _isImporting.asStateFlow()

    private val _importDetectedItems = MutableStateFlow<List<DetectedImportItem>>(emptyList())
    val importDetectedItems: StateFlow<List<DetectedImportItem>> = _importDetectedItems.asStateFlow()

    private var conversionJob: Job? = null

    init {
        // Clean temporary files from previous runs (never touches permanent files in filesDir/stickers)
        viewModelScope.launch(Dispatchers.IO) {
            tempFileManager.cleanTempDirectory()
        }
    }

    fun selectPack(packId: Long) {
        viewModelScope.launch {
            _currentPack.value = packRepository.getPackById(packId)
            stickerRepository.getStickersByPackIdFlow(packId).collect { list ->
                _currentStickers.value = list
            }
        }
    }

    fun createPack(
        name: String,
        shortName: String,
        author: String,
        defaultEmoji: String,
        onCreated: (Long) -> Unit
    ) {
        viewModelScope.launch {
            val pack = StickerPack(
                name = name.ifBlank { "حزمة ملصقات جديدة" },
                shortName = shortName.ifBlank { "pack_" + System.currentTimeMillis() },
                author = author,
                defaultEmoji = defaultEmoji.ifBlank { "✨" }
            )
            val newId = packRepository.createPack(pack)
            selectPack(newId)
            onCreated(newId)
        }
    }

    fun updatePack(pack: StickerPack) {
        viewModelScope.launch {
            packRepository.updatePack(pack)
            if (_currentPack.value?.id == pack.id) {
                _currentPack.value = pack
            }
        }
    }

    fun deletePack(packId: Long) {
        viewModelScope.launch {
            packRepository.deletePack(packId)
            tempFileManager.deletePackFiles(packId)
            if (_currentPack.value?.id == packId) {
                _currentPack.value = null
                _currentStickers.value = emptyList()
            }
        }
    }

    fun inspectUris(uris: List<Uri>) {
        viewModelScope.launch(Dispatchers.IO) {
            _isImporting.value = true
            val items = mutableListOf<DetectedImportItem>()
            val context = getApplication<Application>()

            for (uri in uris) {
                try {
                    val temp = tempFileManager.createTempFile("inspect_", ".tmp")
                    context.contentResolver.openInputStream(uri)?.use { inStream ->
                        FileOutputStream(temp).use { outStream ->
                            inStream.copyTo(outStream)
                        }
                    }

                    val detected = MediaDetector.detect(temp)
                    val rawName = uri.lastPathSegment ?: "sticker"
                    val cleanName = File(rawName).nameWithoutExtension

                    items.add(
                        DetectedImportItem(
                            uriString = uri.toString(),
                            name = cleanName,
                            type = detected.type,
                            sizeBytes = temp.length(),
                            width = detected.width,
                            height = detected.height,
                            durationMs = detected.durationMs
                        )
                    )
                    temp.delete()
                } catch (e: Exception) {
                    DiagnosticsRepository.logError(
                        context = context,
                        operation = "SAF_INSPECT",
                        stage = DiagnosticStage.IMPORT,
                        fileName = uri.lastPathSegment ?: "unknown",
                        uriString = uri.toString(),
                        exception = e,
                        customMessage = "Failed to inspect selected document: ${e.message}"
                    )
                }
            }
            _importDetectedItems.value = items
            _isImporting.value = false
        }
    }

    fun commitImportToPack(packId: Long, items: List<DetectedImportItem>, onComplete: () -> Unit) {
        viewModelScope.launch(Dispatchers.IO) {
            val context = getApplication<Application>()
            val pack = packRepository.getPackById(packId)
            val defaultEmoji = pack?.defaultEmoji ?: "✨"
            val existing = stickerRepository.getStickersByPackId(packId)
            var currentOrder = existing.size

            val newStickers = mutableListOf<StickerItem>()

            for (item in items) {
                currentOrder++
                try {
                    val ext = when (item.type) {
                        StickerType.ANIMATED_WEBP, StickerType.STATIC_WEBP -> "webp"
                        StickerType.PNG -> "png"
                        StickerType.JPG -> "jpg"
                        StickerType.WEBM -> "webm"
                        else -> "bin"
                    }

                    // Copy SAF stream to permanent managed storage
                    val inputStream = context.contentResolver.openInputStream(Uri.parse(item.uriString))
                    if (inputStream == null) {
                        DiagnosticsRepository.logError(
                            context = context,
                            operation = "IMPORT_COMMIT",
                            stage = DiagnosticStage.URI_OPEN,
                            fileName = item.name,
                            uriString = item.uriString,
                            customMessage = "Could not open stream from selected URI"
                        )
                        continue
                    }

                    val (stableFile, stableUri) = inputStream.use { stream ->
                        tempFileManager.saveStreamToPackOriginals(packId, stream, "${item.name}.$ext")
                    }

                    // Validate that ContentResolver can read the permanent FileProvider URI
                    var canRead = false
                    try {
                        context.contentResolver.openInputStream(stableUri)?.use {
                            canRead = true
                        }
                    } catch (e: Exception) {
                        DiagnosticsRepository.logError(
                            context = context,
                            operation = "IMPORT_COMMIT",
                            stage = DiagnosticStage.URI_OPEN,
                            fileName = stableFile.name,
                            file = stableFile,
                            uriString = stableUri.toString(),
                            exception = e,
                            customMessage = "Failed to verify ContentResolver access to permanent URI"
                        )
                    }

                    if (!canRead) continue

                    newStickers.add(
                        StickerItem(
                            packId = packId,
                            originalUri = stableUri.toString(),
                            originalName = item.name,
                            displayName = item.name,
                            emoji = defaultEmoji,
                            type = item.type,
                            originalSize = stableFile.length(),
                            width = item.width,
                            height = item.height,
                            durationMs = item.durationMs,
                            sortOrder = currentOrder
                        )
                    )
                } catch (e: Exception) {
                    DiagnosticsRepository.logError(
                        context = context,
                        operation = "IMPORT_COMMIT",
                        stage = DiagnosticStage.STORE,
                        fileName = item.name,
                        uriString = item.uriString,
                        exception = e,
                        customMessage = "Failed to store imported sticker to managed storage: ${e.message}"
                    )
                }
            }

            stickerRepository.addStickers(newStickers)
            _importDetectedItems.value = emptyList()
            withContext(Dispatchers.Main) {
                selectPack(packId)
                onComplete()
            }
        }
    }

    fun importZipArchive(
        zipUri: Uri,
        packNameOverride: String?,
        authorOverride: String?,
        onComplete: (Long) -> Unit,
        onError: (String) -> Unit
    ) {
        viewModelScope.launch(Dispatchers.IO) {
            _isImporting.value = true
            val context = getApplication<Application>()
            val extractDir = tempFileManager.createTempDirectory("zip_import_")

            try {
                // 1. Open ZIP Stream
                val inStream = context.contentResolver.openInputStream(zipUri)
                if (inStream == null) {
                    DiagnosticsRepository.logError(
                        context = context,
                        operation = "ZIP_IMPORT",
                        stage = DiagnosticStage.IMPORT,
                        fileName = zipUri.lastPathSegment ?: "archive.zip",
                        uriString = zipUri.toString(),
                        customMessage = "Could not open input stream from ZIP URI"
                    )
                    withContext(Dispatchers.Main) {
                        onError("تعذر فتح ملف الأرشيف")
                        _isImporting.value = false
                    }
                    return@launch
                }

                // 2. Extract into temporary working directory
                val result = try {
                    safeZipExtractor.extract(inStream, extractDir)
                } catch (e: Exception) {
                    DiagnosticsRepository.logError(
                        context = context,
                        operation = "ZIP_IMPORT",
                        stage = DiagnosticStage.EXTRACT,
                        fileName = zipUri.lastPathSegment ?: "archive.zip",
                        exception = e,
                        customMessage = "SafeZipExtractor failed: ${e.message}"
                    )
                    throw e
                }

                if (result.extractedFiles.isEmpty()) {
                    withContext(Dispatchers.Main) {
                        onError("لم يتم العثور على ملصقات صالحة في الملف")
                        _isImporting.value = false
                    }
                    return@launch
                }

                // 3. Determine Pack Details
                val finalPackName = packNameOverride?.ifBlank { null }
                    ?: result.packTitle
                    ?: "حزمة ملصقات ${System.currentTimeMillis() % 1000}"

                val finalAuthor = authorOverride?.ifBlank { null }
                    ?: result.packAuthor
                    ?: ""

                val shortName = "pack_" + (finalPackName.replace("[^\\w]".toRegex(), "_").lowercase())

                val pack = StickerPack(
                    name = finalPackName,
                    shortName = shortName,
                    author = finalAuthor,
                    defaultEmoji = "✨"
                )

                val packId = packRepository.createPack(pack)

                // 4. Move extracted files to PERMANENT managed sticker storage & create stable FileProvider URIs
                val stickersToInsert = mutableListOf<StickerItem>()

                for ((index, extracted) in result.extractedFiles.withIndex()) {
                    val rawFile = extracted.file

                    // Stage: VALIDATE
                    if (!rawFile.exists() || !rawFile.isFile || rawFile.length() == 0L) {
                        DiagnosticsRepository.logError(
                            context = context,
                            operation = "ZIP_IMPORT",
                            stage = DiagnosticStage.VALIDATE,
                            fileName = rawFile.name,
                            file = rawFile,
                            customMessage = "Extracted file does not exist or is empty"
                        )
                        continue
                    }

                    // Stage: STORE (Move/Copy to managed permanent storage)
                    val preferredName = "${extracted.cleanDisplayName}.${rawFile.extension}"
                    val (stableFile, stableUri) = try {
                        tempFileManager.saveToPackOriginals(packId, rawFile, preferredName)
                    } catch (e: Exception) {
                        DiagnosticsRepository.logError(
                            context = context,
                            operation = "ZIP_IMPORT",
                            stage = DiagnosticStage.STORE,
                            fileName = rawFile.name,
                            file = rawFile,
                            exception = e,
                            customMessage = "Failed to copy extracted file to permanent pack storage"
                        )
                        continue
                    }

                    // Stage: URI_VALIDATE & URI_OPEN
                    var isStreamOpenable = false
                    try {
                        context.contentResolver.openInputStream(stableUri)?.use {
                            isStreamOpenable = true
                        }
                    } catch (e: Exception) {
                        DiagnosticsRepository.logError(
                            context = context,
                            operation = "ZIP_IMPORT",
                            stage = DiagnosticStage.URI_OPEN,
                            fileName = stableFile.name,
                            file = stableFile,
                            uriString = stableUri.toString(),
                            exception = e,
                            customMessage = "ContentResolver cannot open generated FileProvider URI"
                        )
                    }

                    if (!isStreamOpenable) {
                        continue
                    }

                    // Detect format from stable file
                    val detected = MediaDetector.detect(stableFile)

                    DiagnosticsRepository.logInfo(
                        operation = "ZIP_IMPORT",
                        stage = DiagnosticStage.STORE,
                        fileName = stableFile.name,
                        message = "Sticker successfully extracted and stored in managed storage: ${stableFile.name} (${detected.type.displayNameAr})"
                    )

                    stickersToInsert.add(
                        StickerItem(
                            packId = packId,
                            originalUri = stableUri.toString(),
                            originalName = extracted.cleanDisplayName,
                            displayName = extracted.cleanDisplayName,
                            emoji = "✨",
                            type = detected.type,
                            originalSize = stableFile.length(),
                            width = detected.width,
                            height = detected.height,
                            durationMs = detected.durationMs,
                            sortOrder = index
                        )
                    )
                }

                if (stickersToInsert.isEmpty()) {
                    withContext(Dispatchers.Main) {
                        onError("تعذر استيراد أي ملصقات صالحة من الأرشيف")
                        _isImporting.value = false
                    }
                    return@launch
                }

                stickerRepository.addStickers(stickersToInsert)

                // Clean up temporary extraction folder now that all files are in permanent storage
                try {
                    extractDir.deleteRecursively()
                } catch (_: Exception) {}

                withContext(Dispatchers.Main) {
                    selectPack(packId)
                    _isImporting.value = false
                    onComplete(packId)
                }
            } catch (e: Exception) {
                DiagnosticsRepository.logError(
                    context = context,
                    operation = "ZIP_IMPORT",
                    stage = DiagnosticStage.IMPORT,
                    fileName = zipUri.lastPathSegment ?: "archive.zip",
                    uriString = zipUri.toString(),
                    exception = e,
                    customMessage = "ZIP import operation failed: ${e.message}"
                )
                withContext(Dispatchers.Main) {
                    onError(e.message ?: "فشل استخراج الأرشيف")
                    _isImporting.value = false
                }
            }
        }
    }

    fun updateStickerName(stickerId: Long, newName: String) {
        viewModelScope.launch {
            val item = stickerRepository.getStickerById(stickerId) ?: return@launch
            stickerRepository.updateSticker(item.copy(displayName = newName))
        }
    }

    fun updateStickerEmoji(stickerId: Long, newEmoji: String) {
        viewModelScope.launch {
            val item = stickerRepository.getStickerById(stickerId) ?: return@launch
            stickerRepository.updateSticker(item.copy(emoji = newEmoji))
        }
    }

    fun generateNamesAutomatically(prefix: String = "ملصق") {
        viewModelScope.launch {
            val current = _currentStickers.value
            val updated = current.mapIndexed { index, item ->
                val autoName = String.format(java.util.Locale.US, "%s %03d", prefix, index + 1)
                item.copy(displayName = autoName)
            }
            stickerRepository.updateStickers(updated)
        }
    }

    fun deleteSticker(stickerId: Long) {
        viewModelScope.launch {
            val item = stickerRepository.getStickerById(stickerId)
            if (item != null) {
                try {
                    val uri = Uri.parse(item.originalUri)
                    if (uri.scheme == "file") {
                        File(uri.path ?: "").delete()
                    }
                } catch (_: Exception) {}
                stickerRepository.deleteSticker(stickerId)
            }
        }
    }

    fun moveStickerUp(stickerId: Long) {
        val list = _currentStickers.value.toMutableList()
        val index = list.indexOfFirst { it.id == stickerId }
        if (index > 0) {
            val item = list.removeAt(index)
            list.add(index - 1, item)
            viewModelScope.launch {
                stickerRepository.reorderStickers(list.map { it.id })
            }
        }
    }

    fun moveStickerDown(stickerId: Long) {
        val list = _currentStickers.value.toMutableList()
        val index = list.indexOfFirst { it.id == stickerId }
        if (index >= 0 && index < list.size - 1) {
            val item = list.removeAt(index)
            list.add(index + 1, item)
            viewModelScope.launch {
                stickerRepository.reorderStickers(list.map { it.id })
            }
        }
    }

    fun startConversion(stickers: List<StickerItem>) {
        if (conversionJob?.isActive == true) return

        conversionJob = viewModelScope.launch {
            conversionEngine.convertBatch(stickers) { progress ->
                _conversionProgress.value = progress
            }
        }
    }

    fun reprocessSingleSticker(stickerId: Long) {
        viewModelScope.launch {
            val item = stickerRepository.getStickerById(stickerId) ?: return@launch
            stickerRepository.updateSticker(item.copy(conversionStatus = ConversionStatus.PROCESSING))
            conversionEngine.convertSticker(item)
        }
    }

    fun cancelConversion() {
        conversionJob?.cancel()
        _conversionProgress.value = null
    }

    fun dismissConversionDialog() {
        _conversionProgress.value = null
    }

    fun validateSticker(sticker: StickerItem): ValidationResult {
        val path = sticker.outputPath
        return if (path != null && File(path).exists()) {
            TelegramStickerValidator.validate(File(path), sticker.type.isAnimated)
        } else {
            ValidationResult(isValid = false, errors = listOf("لم يتم التحويل بعد"))
        }
    }
}
