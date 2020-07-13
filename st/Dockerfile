FROM clowder/pyclowder-python3:onbuild
ARG clowder_url
ENV RABBITMQ_URI=amqp://guest:guest@${clowder_url}

ENV ST_WRITE_PWD=replaceMe
ENV MAIN_SCRIPT="st_extractor.py"
