FROM python:3.11-slim

# 1. Crear un usuario de sistema sin privilegios de root
RUN useradd -m edugrade_user

WORKDIR /code

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# 2. Cambiar la propiedad de los archivos al nuevo usuario
RUN chown -R edugrade_user:edugrade_user /code

# 3. Cambiar al usuario seguro antes de ejecutar la app
USER edugrade_user

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]