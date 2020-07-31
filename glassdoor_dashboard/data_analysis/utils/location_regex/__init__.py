import re


def get_state(col):
    """
    A simple regex function to retrieve the US state from the location column
    Location is present in 3 types of formats:
     - Boston, MA
     - Toronto, ON (Canada)
     - Dubai (UAE)
    :param str col: location column passed through the dask dataframe
    :return: "" if missing data or country is present
             state - if no country is present (US)
    """
    state_regex = re.compile(r",([A-Za-z0-9_\.\s]+)")
    country_regex = re.compile(r"\(([\sA-Za-z0-9_\.\s]+)\)")

    # handle empty rows
    if col is "":
        return ""
    # if country exists return empty strings
    if bool(country_regex.search(col)):
        return ""
    # else return the state
    else:
        return re.search(state_regex, col).group(1)


def get_country(col):
    """
    A simple regex function to retrieve the country from the location column
    Location is present in 3 types of formats:
     - Boston, MA
     - Toronto, ON (Canada)
     - Dubai (UAE)
    :param str col: location column passed through the dask dataframe
    :return: country
    """
    country_regex = re.compile(r"\(([\sA-Za-z0-9_\.\s]+)\)")
    country = (
        # extract country from brackets if it exists
        re.search(country_regex, col).group(1)
        if bool(country_regex.search(col))
        # if country in brackets is not present assign country as USA
        else "USA"
    )
    return country
