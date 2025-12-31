@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo ===================================================
echo     ЗАПУСК СЕРВИСА COMPANY-REGISTRY-LT
echo ===================================================

:: --- НАСТРОЙКИ ПУТЕЙ ---
:: Укажите здесь точный путь к вашему python.exe
:: Если он лежит просто в корне p313, то так:
set PYTHON_EXE=c:\p313\python.exe

:: Если python.exe лежит глубже, например c:\p313\python\python.exe, 
:: исправьте строчку выше.
:: -----------------------

:: Проверка, что Python найден по указанному пути
if not exist "%PYTHON_EXE%" (
    echo ОШИБКА: Файл python.exe не найден по пути:
    echo %PYTHON_EXE%
    echo Пожалуйста, отредактируйте файл run.bat и укажите верный путь.
    pause
    exit /b
)

:: 1. Проверяем, есть ли виртуальное окружение
if not exist "venv" (
    echo [SETUP] Виртуальное окружение не найдено. Создаем...
    "%PYTHON_EXE%" -m venv venv
    if errorlevel 1 (
        echo ОШИБКА: Не удалось создать venv.
        pause
        exit /b
    )
    echo [SETUP] Окружение создано.
)

:: 2. Активируем виртуальное окружение
:: (Здесь путь относительный, он создался внутри папки проекта)
call venv\Scripts\activate

:: 3. Обновляем библиотеки (используем pip внутри venv)
echo [SETUP] Проверка библиотек...
pip install -r requirements.txt >nul
if errorlevel 1 (
    echo ОШИБКА: Не удалось установить библиотеки.
    pause
    exit /b
)

:: 4. Запускаем сервис
echo.
echo [START] Запускаем сервер...
echo Доступ по локальной сети: http://localhost:8010/
echo.

:: Запускаем через Python внутри проекта
python -m app.main

echo.
echo [STOP] Сервис остановлен.
pause