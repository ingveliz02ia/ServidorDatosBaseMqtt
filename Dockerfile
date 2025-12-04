FROM python:3.13.8

WORKDIR /Data

COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 80

CMD python servidordatos.py