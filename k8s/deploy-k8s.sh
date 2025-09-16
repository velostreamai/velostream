#!/bin/bash

# VeloStream SQL Kubernetes Deployment Script
set -e

echo "🚀 VeloStream SQL Kubernetes Deployment"
echo "========================================="

# Configuration
NAMESPACE="velo-sql"
KUBECTL_TIMEOUT="300s"
CLEANUP=false
BUILD_IMAGES=true

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --cleanup)
            CLEANUP=true
            echo "🧹 Cleanup mode enabled"
            shift
            ;;
        --no-build)
            BUILD_IMAGES=false
            echo "🏗️ Skipping image build"
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  --cleanup     Remove all Kubernetes resources"
            echo "  --no-build    Skip building Docker images"
            echo "  --help        Show this help"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Cleanup if requested
if [ "$CLEANUP" = true ]; then
    echo "🧹 Cleaning up Kubernetes resources..."
    kubectl delete namespace $NAMESPACE --ignore-not-found=true
    echo "✅ Cleanup completed"
    exit 0
fi

# Check prerequisites
echo "🔍 Checking prerequisites..."

if ! command -v kubectl &> /dev/null; then
    echo "❌ kubectl not found. Please install kubectl first."
    exit 1
fi

if ! command -v docker &> /dev/null; then
    echo "❌ Docker not found. Please install Docker first."
    exit 1
fi

# Check kubectl connectivity
if ! kubectl cluster-info &> /dev/null; then
    echo "❌ Unable to connect to Kubernetes cluster. Please check your kubeconfig."
    exit 1
fi

echo "✅ Prerequisites check passed"

# Build Docker images if requested
if [ "$BUILD_IMAGES" = true ]; then
    echo "🔨 Building Docker images..."
    
    # Build main SQL server image
    docker build -t velo-sql:latest -f Dockerfile .
    if [ $? -ne 0 ]; then
        echo "❌ Failed to build velo-sql image"
        exit 1
    fi
    
    # Build multi-job SQL server image
    docker build -t velo-sql-multi:latest -f Dockerfile.multi .
    if [ $? -ne 0 ]; then
        echo "❌ Failed to build velo-sql-multi image"
        exit 1
    fi
    
    echo "✅ Docker images built successfully"
    
    # Load images into kind cluster if using kind
    if kubectl config current-context | grep -q "kind"; then
        echo "🔄 Loading images into kind cluster..."
        kind load docker-image velo-sql:latest
        kind load docker-image velo-sql-multi:latest
        echo "✅ Images loaded into kind cluster"
    fi
fi

# Create namespace
echo "📦 Creating namespace..."
kubectl apply -f namespace.yaml

# Apply Kubernetes manifests
echo "🚀 Deploying Kafka infrastructure..."
kubectl apply -f kafka.yaml

# Wait for Kafka to be ready
echo "⏳ Waiting for Kafka to be ready..."
kubectl wait --for=condition=available --timeout=$KUBECTL_TIMEOUT deployment/kafka -n $NAMESPACE

echo "🚀 Deploying SQL servers..."
kubectl apply -f sql-servers.yaml

# Wait for SQL servers to be ready
echo "⏳ Waiting for SQL servers to be ready..."
kubectl wait --for=condition=available --timeout=$KUBECTL_TIMEOUT deployment/velo-sql-single -n $NAMESPACE
kubectl wait --for=condition=available --timeout=$KUBECTL_TIMEOUT deployment/velo-sql-multi -n $NAMESPACE

echo "🌐 Applying ingress configuration..."
kubectl apply -f ingress.yaml

# Check deployment status
echo "🏥 Checking deployment status..."
kubectl get pods -n $NAMESPACE
kubectl get services -n $NAMESPACE

# Display connection information
echo ""
echo "🎉 Deployment Complete!"
echo "====================="
echo ""
echo "📋 Services Deployed:"
echo "  • Namespace:           $NAMESPACE"
echo "  • Kafka:               kafka:9092"
echo "  • SQL Single Server:   velo-sql-single:8080"
echo "  • SQL Multi Server:    velo-sql-multi:8080"
echo ""

# Check for NodePort services
NODEPORT_SINGLE=$(kubectl get svc velo-sql-nodeport -n $NAMESPACE -o jsonpath='{.spec.ports[0].nodePort}' 2>/dev/null || echo "")
if [ ! -z "$NODEPORT_SINGLE" ]; then
    echo "🌐 NodePort Access:"
    echo "  • SQL Single API:      http://localhost:$NODEPORT_SINGLE"
    NODEPORT_METRICS=$(kubectl get svc velo-sql-nodeport -n $NAMESPACE -o jsonpath='{.spec.ports[1].nodePort}' 2>/dev/null || echo "")
    if [ ! -z "$NODEPORT_METRICS" ]; then
        echo "  • SQL Single Metrics:  http://localhost:$NODEPORT_METRICS"
    fi
fi

# Check for LoadBalancer services
LB_IP=$(kubectl get svc velo-sql-loadbalancer -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "")
if [ ! -z "$LB_IP" ]; then
    echo "☁️  LoadBalancer Access:"
    echo "  • SQL Single API:      http://$LB_IP:8080"
    echo "  • SQL Single Metrics:  http://$LB_IP:9090"
fi

echo ""
echo "🔧 Management Commands:"
echo "  • View pods:           kubectl get pods -n $NAMESPACE"
echo "  • View services:       kubectl get svc -n $NAMESPACE"
echo "  • View logs:           kubectl logs -f deployment/velo-sql-single -n $NAMESPACE"
echo "  • Scale deployment:    kubectl scale deployment/velo-sql-single --replicas=3 -n $NAMESPACE"
echo ""

echo "📚 Example Usage:"
echo ""
echo "1. Execute SQL query via kubectl:"
echo "   kubectl exec -it deployment/velo-sql-single -n $NAMESPACE -- \\"
echo "     velo-sql execute \\"
echo "     --query \"SELECT * FROM orders WHERE amount > 100\" \\"
echo "     --topic orders \\"
echo "     --brokers kafka:9092"
echo ""
echo "2. Port forward for local access:"
echo "   kubectl port-forward svc/velo-sql-single 8080:8080 -n $NAMESPACE &"
echo "   # Then access: http://localhost:8080"
echo ""
echo "3. Deploy SQL application:"
echo "   kubectl exec -it deployment/velo-sql-multi -n $NAMESPACE -- \\"
echo "     velo-sql-multi deploy-app \\"
echo "     --file /app/examples/ecommerce_analytics.sql \\"
echo "     --brokers kafka:9092 \\"
echo "     --default-topic orders"
echo ""

echo "🎯 Next Steps:"
echo "  1. Create test topics and sample data"
echo "  2. Deploy SQL applications from examples"
echo "  3. Monitor through kubectl logs and metrics"
echo "  4. Scale deployments based on workload"
echo ""

echo "📖 Documentation:"
echo "  • Kubernetes manifests:    k8s/"
echo "  • Docker Deployment:       docs/DOCKER_DEPLOYMENT_GUIDE.md"
echo "  • SQL Reference:           docs/SQL_REFERENCE_GUIDE.md"
echo ""

echo "🎊 VeloStream SQL is now running on Kubernetes!"