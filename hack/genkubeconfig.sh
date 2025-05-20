set -euo pipefail

# Get the script's directory
SCRIPT_DIR=$(dirname "$(realpath "$0")")

# Get the parent directory of SCRIPT_DIR
PARENT_DIR=$(dirname "$SCRIPT_DIR")

GEN_DIR="$PARENT_DIR/gen"

if [[ -z "${LANDSCAPE:-}" ]]; then
  echo "Error: LANDSCAPE is not set"
  exit 1
fi

if [[ -z "${PROJECT:-}" ]]; then
  echo "Error: PROJECT is not set"
  exit 1
fi

if [[ -z "${SHOOT:-}" ]]; then
  echo "Error: SHOOT is not set"
  exit 1
fi

echo "Targeting garden cluster..."
gardenctl target --garden "$LANDSCAPE"

echo "Setting up kubectl environment..."
eval "$(gardenctl kubectl-env zsh)"

echo "✅ kubectl environment is now configured inside this script."

export NAMESPACE=garden-$PROJECT
echo "Generating viewer kubeconfig for shoot: $SHOOT and project namespace: $NAMESPACE ..."
vkcfg=$(kubectl create \
    -f <(printf '{"spec":{"expirationSeconds":86400}}') \
    --raw /apis/core.gardener.cloud/v1beta1/namespaces/${NAMESPACE}/shoots/${SHOOT}/viewerkubeconfig | \
    jq -r ".status.kubeconfig" | \
    base64 -d)
clusterName=$(echo "$vkcfg" | yq ".current-context")
echo "Cluster Name is: $clusterName"

echo "Gen dir is $GEN_DIR"
clusterKubeConfig="$GEN_DIR/$clusterName.yaml"
mkdir -p "$GEN_DIR"
echo "$vkcfg" > "$clusterKubeConfig"
echo "Generated $clusterKubeConfig"