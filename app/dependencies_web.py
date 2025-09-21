import json

from fastapi import Request, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app import models
from app.database import get_db
from auth.auth_handler import decode_jwt


def get_current_user(request: Request, db: Session = Depends(get_db)):
    token_session = request.cookies.get('token')
    if not token_session:
        request.session['not_authorized'] = {
            "status":status.HTTP_404_NOT_FOUND,
            "message":"Not authorized"
        }
        raise HTTPException(
            status_code=status.HTTP_303_SEE_OTHER,
            detail="Not authorized",
            headers={"Location": "/web/guest/login"}
        )
    payload = decode_jwt(token_session)
    if not payload:
        request.session['token_expired'] = {
            "status": status.HTTP_404_NOT_FOUND,
            "message": "Votre token est expiré"
        }
        request.session.clear()
        raise HTTPException(
            status_code=status.HTTP_303_SEE_OTHER,
            detail="Not authorized",
            headers={"Location": "/web/guest/login"}
        )

    user = db.query(models.User).filter(models.User.email == payload.get('user_id')).first()
    if not user:
        request.session['not_authorized'] = {
            "status": status.HTTP_404_NOT_FOUND,
            "message": "Not authorized"
        }
        raise HTTPException(
            status_code=status.HTTP_303_SEE_OTHER,
            detail="Not authorized",
            headers={"Location": "/web/guest/login"}
        )
    return user


def get_admin_user(request:Request, user=Depends(get_current_user)):
    if user.role.value != "admin":
        request.session['not_authorized'] = {
            "status": status.HTTP_404_NOT_FOUND,
            "message": "Not authorized"
        }
        raise HTTPException(
            status_code=status.HTTP_303_SEE_OTHER,
            detail="Not authorized",
            headers={"Location": "/"}
        )
    return user
