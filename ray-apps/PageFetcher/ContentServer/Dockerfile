# Create a simple content server that serves static pages on 
# port 80 with path: /files/filex.dat, where x is a number from 1 to 1000
FROM nginx

COPY generate_files.sh /public/
COPY content_server.conf /etc/nginx/conf.d/
RUN rm /etc/nginx/conf.d/default.conf

RUN chmod +x /public/generate_files.sh
RUN /public/generate_files.sh 50 10M
# RUN /public/generate_files.sh 5 100M

EXPOSE 80