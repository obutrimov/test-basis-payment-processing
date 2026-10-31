# 1. Base image
FROM python:3.11-slim

# 2. Set working directory
WORKDIR /app

# 3. Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# 4. Install poetry
RUN pip install --no-cache-dir poetry==1.8.2

# 5. Copy dependency files
COPY poetry.lock pyproject.toml ./

# 6. Install dependencies
RUN poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-ansi --no-root

# 7. Copy application code
COPY ./tests ./tests
COPY alembic.ini .
COPY migrations ./migrations
COPY ./src ./src

# 8. Expose port
EXPOSE 8000