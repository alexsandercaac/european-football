# Build image
sudo docker build -t sofifa .

# Running container
sudo docker run -d --network=host sofifa