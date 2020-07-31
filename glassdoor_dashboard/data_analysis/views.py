from django.shortcuts import render

from plotly.offline import plot
from luigi import build
import plotly.express as px

from .tasks import (
    QueryRatingsByCountry,
    QueryJobInfoOutlook,
    QueryRatings,
    QueryRatingsByState,
)


def index(request):
    ratings_dict = ratings()
    plot_count_satisfaction = plot_satisfaction()
    plot_count_outlook = plot_outlook()
    ratings_country_dict = ratings_by_country()
    ratings_state_dict = ratings_by_state()
    context = dict(
        **ratings_dict,
        **plot_count_satisfaction,
        **plot_count_outlook,
        **ratings_country_dict,
        **ratings_state_dict
    )

    return render(request, template_name="index.html", context=context)


def ratings():
    # build luigi task
    query = QueryRatings()
    build(
        [query], local_scheduler=True,
    )

    # get mean values of ratings column
    ddf = query.get_output().mean().round(1)

    return {
        "cultureandvaluesrating": ddf.cultureandvaluesrating,
        "seniorleadershiprating": ddf.seniorleadershiprating,
        "compensationandbenefitsrating": ddf.compensationandbenefitsrating,
        "careeropportunitiesrating": ddf.careeropportunitiesrating,
        "worklifebalancerating": ddf.worklifebalancerating,
        "overallnumeric": ddf.overallnumeric,
        "ceorating": ddf.ceorating,
    }


def ratings_by_country():
    # build luigi task
    query = QueryRatingsByCountry()
    build([query], local_scheduler=True)

    # return dataframe that is ordered in descending order of count column
    ddf = query.get_output().sort_values("count", ascending=False)
    return {"ratings_by_country": ddf}


def plot_satisfaction():
    # build luigi task
    query = QueryJobInfoOutlook()
    build([query], local_scheduler=True)

    # only extract overall satisfaction and count column
    ddf = query.get_output()[["overall", "count"]]

    # get the total number of reviews for each response
    ddf = ddf.groupby("overall")[["count"]].sum()

    # plot a bar chart displaying the aggregated analysis
    fig = px.bar(ddf, x=ddf.index, y="count")
    plot_div = plot(fig, output_type="div", config={"displayModeBar": False})
    return {"plot_div": plot_div}


def plot_outlook():
    # build luigi task
    query = QueryJobInfoOutlook()
    build([query], local_scheduler=True)

    # only extract the required columns
    ddf = query.get_output()[["businessoutlook", "count"]]

    # get the total number of reviews for each reponse
    ddf = ddf.groupby("businessoutlook")[["count"]].sum()

    # plot the aggregated results
    fig = px.bar(ddf, x=ddf.index, y="count")
    plot_div = plot(fig, output_type="div", config={"displayModeBar": False})
    return {"plot_outlook": plot_div}


def ratings_by_state():
    # build luigi task
    query = QueryRatingsByState()
    build([query], local_scheduler=True)

    # return dataframe that is ordered in descending order of count column
    ddf = query.get_output().sort_values("count", ascending=False)
    return {"ratings_by_state": ddf}
