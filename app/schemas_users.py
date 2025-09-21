from datetime import datetime
from typing import Optional

from pydantic import BaseModel, EmailStr, Field


class UserRegisterSchemas(BaseModel):
    fullname:str = Field(...)
    email:EmailStr = Field(...)
    password:str = Field(...)
    confirm_password:str = Field(...)

class UserUpdateSchemas(BaseModel):
    role: str = Field(...)

class UserLoginSchemas(BaseModel):
    email:EmailStr = Field(...)
    password:str = Field(...)

class UserResponse(BaseModel):
    id: int
    fullname: str
    role: Optional[str]
    email: EmailStr
    created_at: datetime

    class Config:
        from_attributes = True

class UserResponseAfterUpdate(BaseModel):
    status: int
    message: str
    user: UserResponse

    class Config:
        from_attributes = True

