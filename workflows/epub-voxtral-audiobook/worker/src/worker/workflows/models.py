"""Input / output models for the epub-voxtral-audiobook workflow."""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from worker.workflows.cast_logic import DEFAULT_NARRATOR_VOICE


class EpubAudiobookInput(BaseModel):
    epub_file_id: str = Field(description="Google Drive file ID of the .epub to convert")
    output_folder_id: Optional[str] = Field(
        default=None,
        description="Drive folder to write outputs into; when omitted, a per-run folder "
        "'epub-audiobook-output/<book title>' is created in the caller's My Drive",
    )
    narrator_voice: str = Field(default=DEFAULT_NARRATOR_VOICE, description="Voxtral voice_id for the narrator")
    voice_overrides: dict[str, str] = Field(
        default_factory=dict,
        description="character name -> Voxtral voice_id; wins over automatic casting",
    )
    max_characters: int = Field(default=8, ge=1, le=10, description="max distinct voiced characters (rest fold into narrator)")
    script_model: str = Field(default="mistral-large-latest", description="chat model for character scan + segmentation")
    tts_model: str = Field(default="voxtral-mini-tts-3-0", description="Voxtral model for POST /v1/audio/speech")
    response_format: str = Field(default="mp3", description="audio/speech response_format: mp3, wav, flac, opus, pcm")
    max_epub_bytes: int = Field(default=8_000_000, description="fail early on epubs larger than this")
    max_book_chars: int = Field(default=1_500_000, description="fail early on books whose extracted text exceeds this")
    max_merged_bytes: int = Field(default=30_000_000, description="skip the merged file when it would exceed this")
    max_part_bytes: int = Field(default=12_000_000, description="split an output file into parts above this size")
    chunk_chars: int = Field(default=3_000, ge=200, description="max characters per single TTS request")
    batch_chars: int = Field(default=20_000, ge=2_000, description="characters per batch in the full-book character scan")
    tts_concurrency: int = Field(default=4, ge=1, le=8, description="parallel TTS requests within a chapter")


class DriveFile(BaseModel):
    name: str
    id: str
    view_url: Optional[str] = None
    mime_type: Optional[str] = None
    size_bytes: Optional[int] = None


class CastMember(BaseModel):
    name: str
    description: str = ""
    gender: str = ""
    mentions: int = 0
    voice_id: str
    voice_name: str = ""
    is_narrator: bool = False


class ChapterResult(BaseModel):
    index: int
    title: str
    chars: int = 0
    segments: int = 0
    duration_seconds: Optional[float] = None
    files: list[DriveFile] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class EpubAudiobookOutput(BaseModel):
    book_title: str
    epub_file_id: str
    output_folder_id: str
    cast: list[CastMember]
    chapters: list[ChapterResult]
    files: list[DriveFile]
    merged_file: Optional[DriveFile] = None
    script_file: Optional[DriveFile] = None
    manifest_file: Optional[DriveFile] = None
    stats: dict = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)
