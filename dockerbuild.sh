echo "Stopping containers"

for i in 1 2 3 4
do
  echo "stop st." $i
  docker stop st.$i
  echo "stop yml." $i
  docker stop yml.$i
done

docker stop csv
docker stop wq_csv

echo "Containers stopped"
VERSION=0.4
CLOWDER_URL=34.106.253.53

echo "Build containers"
docker build --build-arg clowder_url=$CLOWDER_URL --tag csv:$VERSION ./csv
docker build --build-arg clowder_url=$CLOWDER_URL --tag st:$VERSION ./st
docker build --build-arg clowder_url=$CLOWDER_URL --tag yml:$VERSION ./yml
docker build --build-arg clowder_url=$CLOWDER_URL --tag wq_csv:$VERSION ./wq_csv

docker rm csv
docker rm st
docker rm yml
docker rm wq_csv

echo "Run Containers"
docker run -d --name csv csv:$VERSION
docker run -d --name st st:$VERSION
docker run -d --name yml yml:$VERSION
docker run -d --name wq_csv wq_csv:$VERSION

