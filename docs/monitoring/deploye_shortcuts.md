GOOS=linux GOARCH=amd64 CGO_ENABLED=0 GOFLAGS="-mod=mod" go build -ldflags="-s -w" -o bootstrap-new
./cmd/main.go && gcloud compute scp bootstrap-new instance-20260317-201243:/tmp/bootstrap-new
--zone=asia-south1-a --project=ridechain-90ebd && gcloud compute ssh instance-20260317-201243
--zone=asia-south1-a --project=ridechain-90ebd --command='set -e; sudo cp
/opt/ridechain-bootstrap/bootstrap /opt/ridechain-bootstrap/bootstrap.bak || true; sudo mv
/tmp/bootstrap-new /opt/ridechain-bootstrap/bootstrap; sudo chmod +x
/opt/ridechain-bootstrap/bootstrap; sudo chown ridechain:ridechain
/opt/ridechain-bootstrap/bootstrap; sudo systemctl restart ridechain-bootstrap; sleep 3; echo "===
systemctl ==="; sudo systemctl status ridechain-bootstrap --no-pager; echo; echo "=== recent
logs ==="; sudo journalctl -u ridechain-bootstrap -n 30 --no-pager'



============ Working=========
cd /Users/umashankar.pathak/Documents/Learn_Node/ride/bootstrap

GOOS=linux GOARCH=amd64 CGO_ENABLED=0 GOFLAGS="-mod=mod" \
go build -ldflags="-s -w" -o bootstrap-new ./cmd

gcloud compute scp bootstrap-new instance-20260317-201243:/tmp/bootstrap-new --zone=asia-south1-a --project=ridechain-90ebd && gcloud compute ssh instance-20260317-201243 --zone=asia-south1-a --project=ridechain-90ebd --command='set -e; sudo cp /opt/ridechain-bootstrap/bootstrap /opt/ridechain-bootstrap/bootstrap.bak || true; sudo mv /tmp/bootstrap-new /opt/ridechain-bootstrap/bootstrap; sudo chmod +x /opt/ridechain-bootstrap/bootstrap; sudo chown ridechain:ridechain /opt/ridechain-bootstrap/bootstrap; sudo systemctl restart ridechain-bootstrap; sleep 3; echo "=== systemctl ==="; sudo systemctl status ridechain-bootstrap --no-pager; echo; echo "=== recent logs ==="; sudo journalctl -u ridechain-bootstrap -n 30 --no-pager'


SAVE THIS FOR BOOTSTRAP TURN_SHARED_SECRET: 0c1376fa60c23ef1263788374ae7c2b231b3a730f20f53d9eff329d1846c12ab

