# Testing Unofficial TikTok API

from TikTokApi import TikTokApi
from pathlib import Path
api1 = TikTokApi.get_instance()
# If playwright doesn't work for you try to use selenium
api2 = TikTokApi.get_instance(use_selenium=True)

base_path = Path(__file__).parent
file_path = (base_path / "../utilities/chromedriver.exe").resolve()
api3 = TikTokApi.get_instance(use_selenium=True, executablePath=file_path)

results = 10

# Since TikTok changed their API you need to use the custom_verifyFp option.
# In your web browser you will need to go to TikTok, Log in and get the s_v_web_id value.
trending1 = api1.trending(count=results, custom_verifyFp="verify_klat6pua_gX3v9ItE_uqdV_4zPu_8rMk_KIMu3i51EFuI")
trending2 = api2.trending(count=results, custom_verifyFp="verify_klat6pua_gX3v9ItE_uqdV_4zPu_8rMk_KIMu3i51EFuI")
#trending3 = api3.trending(count=results, custom_verifyFP="verify_klat6pua_gX3v9ItE_uqdV_4zPu_8rMk_KIMu3i51EFuI")

trending3 = api2.trending(count=results, custom_verifyFp="verify_klkw2don_kCWTFtWb_U1Qu_4OZl_8Rhq_r1fUbV5QMKIt")


userID = "6717651461067604997"
secUID = "MS4wLjABAAAALP9H8t1_SVmfuAKXV1o9K8XqiaFLxm2ae-EJ5_AJcwogcI_d9btuf_fjbjFOMNpN"
posts = api2.userPosts(userID=userID, secUID=secUID, custom_verifyFp="verify_kljnnr1d_aPqkxu8I_TXtT_4xO8_8zE5_jEkg97g2DRqO")

for tiktok in trending1:
    # Prints the id of the tiktok
    print(tiktok['id'])

print(len(trending1))

for tiktok in trending2:
    # Prints the id of the tiktok
    print(tiktok['id'])

print(len(trending2))