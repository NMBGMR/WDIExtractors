echo "Stopping containers"

for i in 1 2 3 4
do
  echo "stop st." $i
  docker stop st.$i
  echo "stop validator." $i
  docker stop validator.$i
done

docker stop wq_csv
docker stop csv

echo "Containers stopped"
VERSION=0.5
CLOWDER_URL=34.106.253.53

echo "Build containers"
docker build --build-arg clowder_url=$CLOWDER_URL --tag csv:$VERSION ./csv
docker build --build-arg clowder_url=$CLOWDER_URL --tag st:$VERSION ./st
docker build --build-arg clowder_url=$CLOWDER_URL --tag validator:$VERSION ./validator
docker build --build-arg clowder_url=$CLOWDER_URL --tag wq_csv:$VERSION ./wq_csv

docker rm csv
for i in 1 2 3 4
do
  docker rm st.$i
  docker rm validator.$i
done

docker rm csv
docker rm wq_csv

echo "Run Containers"

for i in 1 2 3 4
do
  docker run -d --name yml.$i yml:$VERSION
  docker run -d --name st.$i st:$VERSION
done

docker run -d --name wq_csv wq_csv:$VERSION
docker run -d --name csv csv:$VERSION
