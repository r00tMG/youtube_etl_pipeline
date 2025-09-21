from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer

from auth.auth_handler import decode_jwt

security = HTTPBearer()  # Pour récupérer le token Authorization

def get_current_user(credentials=Depends(security)):
    token = credentials.credentials
    payload = decode_jwt(token).get('user_id')
    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide")
    return payload

def get_admin_user(user=Depends(get_current_user)):
    print("test admin")
    if user.get("role") != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin uniquement")
    return user
