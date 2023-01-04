build:
    docker buildx build --pull --push --platform linux/amd64 -t registry.digitalocean.com/jtdowney/protohackers .