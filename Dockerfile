FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml .
COPY scout/ scout/
RUN pip install --no-cache-dir .[all]
CMD ["scout-slack-listener"]
