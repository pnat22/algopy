FROM python:3.9

# COPY . /src

ENV TZ="Asia/Kolkata"

WORKDIR /src

ADD requirements.txt .

RUN pip install -r requirements.txt

# CMD ["/bin/python3"]

CMD ["python3"]