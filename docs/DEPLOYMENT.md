## Setting up nginx

sudo apt update && sudo apt upgrade -y
sudo apt install nginx -y
sudo systemctl start nginx
sudo systemctl enable nginx

# Create a new configuration file for sensai
sudo vim /etc/nginx/sites-available/sensai
Add the following config:
```
server {
    listen 80;

    # Production site
    server_name sensai.hyperverge.org;

    location / {
        proxy_pass http://localhost:8501;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_cache_bypass $http_upgrade;
    }
}

server {
    listen 80;

    # Development site
    server_name sensai.dev.hyperverge.org;

    location / {
        proxy_pass http://localhost:8502;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_cache_bypass $http_upgrade;
    }
}
```

# Create symlink to sites-enabled
sudo ln -s /etc/nginx/sites-available/sensai /etc/nginx/sites-enabled/

# Remove the default site (optional)
sudo rm /etc/nginx/sites-enabled/default

# Test the configuration
sudo nginx -t

# Reload nginx to apply changes
sudo systemctl reload nginx