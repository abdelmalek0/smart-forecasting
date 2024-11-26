from fasthtml.common import FastHTML

from routes.datapoints import datapoints
from routes.datasources import datasources
from routes.home import home
from utils.ui_constants import alpine
from utils.ui_constants import apex_charts
from utils.ui_constants import daisyui
from utils.ui_constants import datatables_css
from utils.ui_constants import datatables_js
from utils.ui_constants import datetime_picker
from utils.ui_constants import datetime_picker_extras
from utils.ui_constants import datetime_picker_theme
from utils.ui_constants import general_css
from utils.ui_constants import htmx
from utils.ui_constants import jquery
from utils.ui_constants import tailwindcss

app = FastHTML(
    hdrs=(
        htmx,
        tailwindcss,
        daisyui,
        general_css,
        apex_charts,
        alpine,
        jquery,
        datatables_css,
        datatables_js,
        datetime_picker,
        datetime_picker_extras,
        datetime_picker_theme,
    ),
    default_hdrs=False,
)

app.add_route("/", home)
app.add_route("/datasources", datasources)
app.add_route("/datapoints/", datapoints)
app.add_route("/datapoints/{datasource_id}", datapoints)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=3000, reload=True)
