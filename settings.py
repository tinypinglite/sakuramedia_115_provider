from pydantic import BaseModel, ConfigDict, Field


class Cloud115ProviderSettings(BaseModel):
    """115 插件私有配置（plugins.settings.sakuramedia_115_provider）。"""

    model_config = ConfigDict(extra="forbid")

    thumbnail_hls_max_workers: int = Field(
        default=1,
        ge=1,
        le=16,
        title="缩略图 HLS 分片并发数",
        description=(
            "同时下载并解码的 HLS 分片数量，调大可加快缩略图生成，"
            "过高可能触发网盘限速（修改后需重启容器生效）"
        ),
    )
