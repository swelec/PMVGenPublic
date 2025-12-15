FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY main.py \
     move_output.py \
     move2oculus.py \
     music_guided_generator.py \
     scan.py \
     reports.py \
     MUSIC_README.md \
     private_settings.example.py \
     ./ 

CMD ["python", "main.py"]
