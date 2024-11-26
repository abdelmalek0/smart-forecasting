from fasthtml.common import A
from fasthtml.common import Aside
from fasthtml.common import Div
from fasthtml.common import Footer
from fasthtml.common import H1
from fasthtml.common import P
from fasthtml.common import Title
from starlette.requests import Request

from components.navbar import Navbar
from utils.constants import APP_DESCRIPTION
from utils.constants import APP_NAME
from utils.constants import COPYRIGHTS


def home(request: Request):
    """
    Render the home page with a navbar, heading, description, button, and footer.

    :param request: The Starlette request object.
    :return: A Div element containing the home page layout.
    """
    return (
        Title("SmartForecasting - Home"),
        Div(
            Navbar(index=0),
            Div(
                H1(APP_NAME, cls="text-7xl font-bold"),
                P(APP_DESCRIPTION, cls="text-center italic"),
                A(
                    "Get Started",
                    href="/datasources",
                    cls="btn btn-primary text-slate-100",
                ),
                cls="flex flex-1 flex-col justify-center items-center gap-8 mx-[20em]",
            ),
            Footer(
                Aside(P(COPYRIGHTS)),
                cls="footer footer-center text-base-content bg-gray-50 h-10 text-black rounded-md",
            ),
        ),
    )
