FROM python
EXPOSE 5000

COPY . /openshift-ci-trigger
WORKDIR /openshift-ci-trigger
RUN python3 -m pip install pip --upgrade \
    && python3 -m pip install poetry \
    && poetry config cache-dir /app \
    && poetry config virtualenvs.in-project true \
    && poetry config installer.max-workers 10 \
    && poetry config --list \
    && poetry env remove --all \
    && poetry install

ENTRYPOINT ["poetry", "run", "python3", "app/app.py"]
