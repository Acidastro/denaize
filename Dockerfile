FROM python:3

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . /app

WORKDIR /app

EXPOSE 80

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]