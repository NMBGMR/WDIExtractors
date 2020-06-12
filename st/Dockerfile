FROM clowder/pyclowder-python3:onbuild
ARG clowder_url
ENV RABBITMQ_URI=amqp://guest:guest@${clowder_url}

ENV ST_URL=http://st.newmexicowaterdata.org/FROST-Server/v1.1
ENV MAIN_SCRIPT="st_extractor.py"
