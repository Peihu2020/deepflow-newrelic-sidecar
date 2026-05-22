FROM golang:1.26-alpine AS builder

WORKDIR /app

# 设置 Go 环境变量
ENV GO111MODULE=on
ENV GOPROXY=https://goproxy.cn,direct

# 安装依赖
RUN apk add --no-cache git

# 复制依赖文件
COPY go.mod go.sum ./

# 下载依赖
RUN go mod download

# 复制源码
COPY *.go ./

# 编译
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o sidecar .

# 最终镜像
FROM alpine:3.18

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

COPY --from=builder /app/sidecar .

ENTRYPOINT ["./sidecar"]