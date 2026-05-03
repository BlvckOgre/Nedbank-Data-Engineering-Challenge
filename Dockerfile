FROM nedbank-de-challenge/base:1.0

WORKDIR /app

COPY pipeline/ ./pipeline/

CMD ["python", "pipeline/run_all.py"]