FROM mcr.microsoft.com/devcontainers/base:jammy

# Get this thing up-to-date
RUN apt-get update && \
    apt-get upgrade -y && \ 
    apt-get install -y curl 

# Install and configure MISE
RUN curl https://mise.run | MISE_INSTALL_PATH=/usr/local/bin/mise sh && \
    echo 'eval "$(/usr/local/bin/mise activate bash)"' >> /home/vscode/.bashrc  && \
    echo 'eval "$(/usr/local/bin/mise activate bash --shims)"' >> /home/vscode/.bash_profile

# locale
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LANGUAGE en_US:en

CMD ["/bin/bash"]