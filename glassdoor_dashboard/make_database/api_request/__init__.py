import os

# get partnerID and partnerkey to get URL

# #Future improvements to add here - arguments taken in from CLI for all parameters
def get_params(
    v="1",
    format="json",
    userip="",
    useragent="",
    action="employer-review",
    city="",
    pageNumber="1",
    pageSize="100",
    includeReviewText="true",
):
    """
    Takes in parameters for API call
    :return:  parameters to be passed in API call
    :rtype: dict
    """

    return {
        "v": v,
        "format": format,
        "t.p": os.environ["PARTNER_ID"],
        "t.k": os.environ["PARTNER_KEY"],
        "userip": userip,
        "useragent": useragent,
        "action": action,
        "employerId": os.environ["EMPLOYER_ID"],
        "city": city,
        "pageNumber": pageNumber,
        "pageSize": pageSize,
        "includeReviewText": includeReviewText,
    }
