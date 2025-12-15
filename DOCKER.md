# Развёртывание PMVGen в Docker на Synology

## 1. Подготовка
1. Склонируй репозиторий на рабочую машину или NAS.
2. В корне создай каталог `docker-data` (название можешь изменить) и подпапки: `output`, `logs`, `tmp`, `music_projects`, `Music`. Сюда контейнер будет складывать БД, логи и музыку.
3. Скопируй `private_settings.example.py` в `docker-data/private_settings.py` и впиши реальные токены/пути. Можно вместо файла использовать переменные окружения — см. `docker-compose.example.yml`.
4. Если уже есть `pmv_bot.db`, положи его в `docker-data/pmv_bot.db`, иначе файл появится автоматически.

## 2. Сборка образа
```bash
docker build -t pmvgen .
```
`Dockerfile` использует образ `python:3.11-slim`, устанавливает `ffmpeg` и зависимости из `requirements.txt`, затем копирует скрипты.

## 3. Compose (рекомендовано)
Скопируй `docker-compose.example.yml` → `docker-compose.yml` и поправь пути к маунтам/переменным, если нужно. Запуск:
```bash
docker compose up -d
```
Контейнер `pmvgen` будет автоматически рестартовать (`restart: unless-stopped`) и читать конфиг из смонтированного `private_settings.py`.

## 4. Одноразовый запуск без compose
```bash
docker run --rm \
  -v $(pwd)/docker-data/private_settings.py:/app/private_settings.py:ro \
  -v $(pwd)/docker-data/pmv_bot.db:/app/pmv_bot.db \
  -v $(pwd)/docker-data/logs:/app/logs \
  -v $(pwd)/docker-data/output:/app/output \
  -v $(pwd)/docker-data/tmp:/app/tmp \
  -v $(pwd)/docker-data/music_projects:/app/music_projects \
  -v $(pwd)/docker-data/Music:/app/Music \
  pmvgen
```
Добавь `-e TELEGRAM_BOT_TOKEN=...` и т.д., если не монтируешь файл с настройками.

## 5. Настройка на Synology
1. Открой “Container Manager” → “Image” → “Build”/“Import” и собери образ `pmvgen`.  
2. При запуске контейнера:
   - в разделе “Volume” добавь все маунты;
   - в “Environment” укажи таймзону и, при необходимости, секреты;
   - включи автоматический старт.
3. Логи можно смотреть через UI (“Log”) или командой `docker logs -f pmvgen`.

## 6. Обновления
1. Обнови код (`git pull` или загрузка новой версии).  
2. Пересобери образ `docker build -t pmvgen .`.  
3. Перезапусти контейнер `docker compose up -d --build` (или “Redeploy” из UI).

## 7. Проверка
- `docker ps` — убедись, что контейнер работает.  
- `docker exec -it pmvgen bash` — интерактивная оболочка для диагностики.  
- `python -m py_compile main.py` внутри контейнера — быстрый тест на синтаксис.  
- `which ffmpeg` — убедись, что бинарник доступен (`/usr/bin/ffmpeg`).  
