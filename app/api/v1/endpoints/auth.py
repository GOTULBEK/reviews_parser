from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import require_admin
from app.core.security import create_access_token, hash_password, verify_password
from app.db.database import get_session
from app.models.auth import User, UserRole
from app.models.tasks import SearchTask
from app.schemas.auth import LoginRequest, RegisterRequest, TokenResponse

router = APIRouter()


@router.post("/login", response_model=TokenResponse, summary="Obtain a bearer token")
async def login(payload: LoginRequest, session: AsyncSession = Depends(get_session)):
    result = await session.execute(select(User).where(User.login == payload.login))
    user = result.scalar_one_or_none()
    if user is None or not verify_password(payload.password, user.hashed_password):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid credentials")
    token = create_access_token(user.id, user.role, user.task_id)
    return TokenResponse(access_token=token)


@router.post(
    "/register",
    status_code=status.HTTP_201_CREATED,
    summary="Create a customer account for a completed task (admin only)",
    dependencies=[Depends(require_admin)],
)
async def register(payload: RegisterRequest, session: AsyncSession = Depends(get_session)):
    task = await session.get(SearchTask, payload.task_id)
    if task is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Task not found")

    existing = (
        await session.execute(select(User).where(User.login == payload.login))
    ).scalar_one_or_none()
    if existing is not None:
        raise HTTPException(status.HTTP_409_CONFLICT, "Login already taken")

    user = User(
        login=payload.login,
        hashed_password=hash_password(payload.password),
        role=UserRole.customer,
        task_id=payload.task_id,
    )
    session.add(user)
    await session.commit()
    return {"message": "Customer created successfully"}
