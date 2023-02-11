from python:3.8

RUN apt-get update -y && apt-get upgrade -y

COPY . .

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

CMD python3 sofifa_get_players.py