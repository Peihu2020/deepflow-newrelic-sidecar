# Clean previous build
rm -f deepflow_newrelic.so

# Build using Rocky Linux 8 (glibc 2.28)
docker run --rm -v $(pwd):/build -w /build rockylinux:8 bash -c "
    # Update CA certificates and install git
    dnf update -y ca-certificates
    dnf install -y git golang
    
    # Fix go.mod version
    sed -i 's/go 1.26.3/go 1.21/g' go.mod
    
    # Disable Go proxy to avoid cert issues
    export GOPROXY=direct
    export GOSUMDB=off
    
    # Download dependencies
    go mod tidy
    
    # Build the plugin
    CGO_ENABLED=1 go build -buildmode=c-shared -o deepflow_newrelic.so main.go
    
    # Show GLIBC version requirement
    echo 'GLIBC requirements:'
    objdump -T deepflow_newrelic.so | grep GLIBC | sed 's/.*GLIBC_\([0-9.]*\).*/\1/' | sort -u
"

if [ -f deepflow_newrelic.so ]; then
    echo "✅ Build successful!"
    ls -lh deepflow_newrelic.so
    # Check GLIBC version
    MAX_GLIBC=$(objdump -T deepflow_newrelic.so | grep GLIBC | sed 's/.*GLIBC_\([0-9.]*\).*/\1/' | sort -V | tail -1)
    echo "Highest GLIBC version required: $MAX_GLIBC"
    if [[ "$MAX_GLIBC" < "2.29" ]]; then
        echo "✅ Compatible with RHEL 8.9 (GLIBC 2.28)"
    else
        echo "⚠️ Requires GLIBC $MAX_GLIBC, may not work on RHEL 8.9"
    fi
else
    echo "❌ Build failed!"
fi
