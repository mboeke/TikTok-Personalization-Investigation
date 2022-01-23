import time

from flask import Flask, request, redirect
from twilio.twiml.messaging_response import MessagingResponse

import os
from twilio.rest import Client

class SMSHandler:
    """
    Using the mobile phone service Twilio through their API this class allows to create new phone number for a specific
    country, receive all SMS from specific phone number, receive newest SMS from specific phone number, filter out
    verification code from a SMS
    """
    def __init__(self, database):
        self.database = database
        account_sid = 'PLACEHOLDER ACCOUNT_SID'
        auth_token = 'PLACEHOLDER AUTH_TOKEN'
        self.client = Client(account_sid, auth_token)

    def get_cheapest_available_phone_number(self, country):
        """
        Get the cheapest available phone number for a country
        :param country:
        :return:
        """

    def create_phone_number(self, country):
        """
        Create phone number for a specific country at the lowest price
        :param country:
        :return:
        """

    def get_all_sms(self, phone_number):
        """
        Receiving all SMS for specific phone number
        :param phone_number:
        :return:
        """
        messages = {}
        for message in self.client.messages.list(to=phone_number):
            messages[str(message.date_created)] = message.body
        return messages

    def get_newest_sms_body(self, phone_number, phone_number_country_prefix_numerous):
        """
        Receiving newest SMS for specific phone number
        :param phone_number_country_prefix_numerous:
        :param phone_number:
        :return:
        """
        time.sleep(10)
        adjusted_phone_number = str(phone_number_country_prefix_numerous) + phone_number
        return self.client.messages.list(to=adjusted_phone_number)[0].body

    def get_verification_code(self, test_user_id, phone_number, phone_number_country_prefix_numerous):
        """
        Return verification code from TikTok SMS. Attention, sometimes Twilio is quite slow, so the bot has to double
        check if received verification code is not already known. If that is the case, the bot has to wait a few more
        seconds.
        :param test_user_id:
        :param phone_number_country_prefix_numerous:
        :param phone_number:
        :return:
        """
        newest_message = self.get_newest_sms_body(phone_number, phone_number_country_prefix_numerous)

        # handle different verification codes
        verification_code = newest_message[9:13]
        if not verification_code.isdigit():
            try:
                idx_start = newest_message.index('use')
                idx_end = newest_message.index('as')
                verification_code = newest_message[idx_start + 3:idx_end].strip()
                code = ''
                for char in verification_code:
                    if char.isdigit():
                        code = code + char
                verification_code = code.strip()
                if not verification_code.isdigit():
                    raise ValueError("Verificaiton Code not digit.")
            except ValueError as e:
                print("SMS: " + newest_message)
                print("Error: no verification code provided by TikTok, resend code.")
                print("Value Error: " + str(e))
                return "Trigger Resend"

        # check if verification different to previous one
        previous_code = self.database.get_previous_verification_code(test_user_id=test_user_id)
        if int(verification_code) == previous_code:
            print(f"Verification code {verification_code} seems to be too old for {test_user_id}, "
                  f"fetching again in 10secs.")
            time.sleep(10)
            self.get_verification_code(test_user_id=test_user_id,
                                       phone_number=phone_number,
                                       phone_number_country_prefix_numerous=phone_number_country_prefix_numerous)
        else:
            self.database.update_verification_code(verification_code=verification_code,
                                                   test_user_id=test_user_id)
            return verification_code


