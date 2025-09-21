import enum

from sqlalchemy import Column, Integer, String, DateTime, func, Enum, ForeignKey, Float
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
    predictions = relationship("Prediction", back_populates="users", cascade="all, delete-orphan")

class Prediction(Base):
    __tablename__ = "predictions"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="Cascade"))
    result = Column(String)
    score = Column(Float)
    users = relationship("User", back_populates="predictions")


