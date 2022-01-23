// from https://www.google.com/url?sa=t&rct=j&q=&esrc=s&source=web&cd=&cad=rja&uact=8&ved=2ahUKEwjJiYiK8arvAhWGCuwKHQWzABgQFjADegQIAhAD&url=https%3A%2F%2Fwonderproxy.com%2Fblog%2Fa-step-by-step-guide-to-setting-up-a-proxy-in-selenium%2F&usg=AOvVaw2u3tm5J7KAE_B2OlkEhK_7

var config = {
    mode: "fixed_servers",
    rules: {
        singleProxy: {
            scheme: "http",
            host: "209.127.191.180",
            port: parseInt(9279)
        },
        bypassList: ["foobar.com"]
    }
};

chrome.proxy.settings.set({value: config, scope: "regular"}, function () {
});

function callbackFn(details) {
    return {
        authCredentials: {
            username: "PLACEHOLDER",
            password: "PLACEHOLDER"
        }
    };
}

chrome.webRequest.onAuthRequired.addListener(
    callbackFn,
    {urls: ["<all_urls>"]},
    ['blocking']
);