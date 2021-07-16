.PHONY: all build push

DOCKER_REGISTRY := ""
IMAGE_NAME      := docker_test
TAG             := latest

ifeq ($(DOCKER_REGISTRY),1)
    IMAGE := $(DOCKER_REGISTRY)/$(IMAGE_NAME):$(TAG)
else
    IMAGE := $(IMAGE_NAME):$(TAG)
endif

build:
	docker build -t $(IMAGE) .

push: build
    docker push $(IMAGE)
