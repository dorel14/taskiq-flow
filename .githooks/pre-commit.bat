@echo off
echo ========================================
echo Taskiq-Flow Pre-Commit Hook
echo ========================================
echo.

REM Check if we should skip the hook
if "%SKIP_PRE_COMMIT%"=="1" (
    echo ⚠️  Pre-commit hook skipped (SKIP_PRE_COMMIT=1)
    exit /b 0
)

echo [1/3] Checking version synchronization...
python scripts\version_updater.py --check
if %ERRORLEVEL% neq 0 (
    echo ❌ Version check failed. Fix version mismatches before committing.
    echo 💡 Run: python scripts\version_updater.py --bump patch
    exit /b 1
)

echo.
echo [2/3] Version is consistent.

REM TODO: Add other pre-commit checks here:
REM - Run formatter (black/isort)
REM - Run linter (ruff)
REM - Run type check (mypy)
REM - Run tests (pytest)

echo.
echo [3/3] All pre-commit checks passed ✅
exit /b 0
