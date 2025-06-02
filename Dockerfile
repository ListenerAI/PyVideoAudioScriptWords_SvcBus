FROM python:3.11-slim

# Establecer una carpeta de trabajo en el contenedor
WORKDIR /app

# Actualizar pip y las herramientas del sistema
RUN pip install --upgrade pip && \
    apt-get update && \
    apt-get install -y \
    gcc \
    libportaudio2 \
    libportaudiocpp0 \
    portaudio19-dev \
    libglib2.0-0 \
    libgl1-mesa-glx \
    libssl-dev \
    build-essential \
    libasound2 \
    wget \
    libsndfile1 \
    ffmpeg \
    libavcodec-dev \
    libavformat-dev \
    libavdevice-dev \
    libavutil-dev \
    libswscale-dev \
    libx264-dev \
    libffi-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copiar el archivo de requerimientos al contenedor
COPY requirements.txt .

# Instalar las dependencias de Python restantes
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el resto de archivos de la aplicación al contenedor
COPY . .

# Asegúrate de que el directorio /data exista y cambia los permisos
RUN mkdir -p /data && \
    chmod -R 777 /data

# Variables de entorno
# ENV SPEECH_KEY=149a326359244a2faa89acf4ad0d4396
# ENV SPEECH_REGION=eastus

# Exponer el puerto que utiliza tu aplicación
EXPOSE 8000

# Comando para iniciar la aplicación
CMD ["python", "main.py"]

RUN pip freeze > /requirements_installed.txt