import enum

from sqlalchemy import Column, Integer, String, DateTime, func, Enum, ForeignKey, Float, JSON
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from app.database import Base

class UserRole(str, enum.Enum):
    ADMIN = "admin"
    USER = "user"

class User(Base):
    __tablename__ ="users"
    id = Column(Integer, primary_key=True, index=True)
    fullname = Column(String)
    email = Column(String)
    password = Column(String)
    role = Column(Enum(UserRole), default=UserRole.USER)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class StagingYoutubeData(Base):
    __tablename__ = "staging_youtube_datas"
    id = Column(Integer, primary_key=True)
    channel_handle = Column(String)
    extraction_date = Column(String)
    total_videos = Column(Integer)
    videos = Column(JSON)
    created_at = Column(DateTime(timezone=True), server_default=func.now())



