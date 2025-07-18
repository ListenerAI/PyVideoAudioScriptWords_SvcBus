FROM python:3.10-slim

WORKDIR /app

# ✅ Install system dependencies needed for ffmpeg and video/audio processing
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libglib2.0-0 \
    libgl1-mesa-glx \
    libssl-dev \
    build-essential \
    libasound2 \
    wget \
    ffmpeg \
    libavcodec-dev \
    libavformat-dev \
    libavdevice-dev \
    libavutil-dev \
    libswscale-dev \
    libx264-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# ✅ Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --default-timeout=120 -r requirements.txt

# ✅ Copy application code
COPY . .

# ✅ Create data directory
RUN mkdir -p /data && chmod -R 777 /data

# ✅ Expose (optional, if using Flask endpoints)
EXPOSE 8000

# ✅ Command to run the Service Bus processing loop
CMD ["python", "main.py"]

# ✅ Freeze installed packages for inspection/debugging
RUN pip freeze > /requirements_installed.txt
