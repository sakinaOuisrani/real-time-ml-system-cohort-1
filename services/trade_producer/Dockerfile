FROM python:3.11-slim-buster

# ENV PYTHONPATH=/app/src

# stream output to the console
ENV PYTHONUNBUFFERED=1

# Install poetry inside the container
RUN pip install poetry==1.8.4

WORKDIR /app

ENV PYTHONPATH "${PYTHONPATH}:/app/src"

COPY pyproject.toml poetry.lock /app/

# Install dependencies from pyproject.toml file
RUN poetry install

# Copy all the source code to the container
COPY . /app/

CMD ["poetry", "run", "python", "src/main.py"]