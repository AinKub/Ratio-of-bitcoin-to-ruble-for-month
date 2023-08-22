FROM python:3.9-slim-bullseye as compile-image 
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
COPY requirements.txt .
RUN apt-get update \ 
    && pip install --no-cache-dir --upgrade pip \ 
    && pip install --no-cache-dir -r requirements.txt

FROM python:3.9-slim-bullseye
WORKDIR /app
COPY --from=compile-image /opt/venv /opt/venv 
ENV PATH="/opt/venv/bin:$PATH"
COPY . /app