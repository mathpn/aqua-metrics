FROM python:3.11-slim

WORKDIR /app

ADD ./* /app/

RUN pip3 install -r requirements.txt

ENTRYPOINT ["streamlit", "run", "/app/main.py", "--server.port=8501", "--server.address=0.0.0.0"]
