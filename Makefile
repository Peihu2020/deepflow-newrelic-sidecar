.PHONY: build docker-build docker-push clean run test

# 变量
APP_NAME=deepflow-newrelic-sidecar
VERSION?=latest
REGISTRY?=your-registry.com

# 构建二进制文件
build:
	go mod tidy
	CGO_ENABLED=0 GOOS=linux go build -o ${APP_NAME} .

# 构建 Docker 镜像
docker-build:
	docker build -t ${REGISTRY}/${APP_NAME}:${VERSION} .

# 推送 Docker 镜像
docker-push:
	docker push ${REGISTRY}/${APP_NAME}:${VERSION}

# 本地运行
run:
	go run .

# 清理
clean:
	rm -f ${APP_NAME}
	rm -f *.log

# 测试
test:
	go test -v ./...

# 部署到 Kubernetes
deploy:
	kubectl apply -f k8s-daemonset.yaml

# 卸载
undeploy:
	kubectl delete -f k8s-daemonset.yaml

# 查看日志
logs:
	kubectl logs -n deepflow -l app=deepflow-sidecar -c newrelic-sidecar --tail=100 -f