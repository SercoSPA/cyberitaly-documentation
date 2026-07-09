# “build configuration file”, contains (almost) all configuration needed to customize Sphinx input and output behavior.
#
# More information at:
# https://www.sphinx-doc.org/en/master/usage/configuration.html
#
# Note:
# Some inspiration here comes from how the Sphinx instance is configured by the creators of the pydata_sphinx_theme in their example,
# pydata_sphinx_theme is the theme adopted by this project.
# https://github.com/pydata/pydata-sphinx-theme/blob/main/docs/conf.py

from datetime import date

PROJECT_NAME = 'IRIDE CyberItaly Documentation'

PROJECT_VERSION = '1.0.2'

# 🛠️ -- Project information -----------------------------------------------------
needs_sphinx = '8.1.3'

project = PROJECT_NAME

author = 'CGI Italy'

copyright = f'{date.today().year}', 'CGI Italy'

version = PROJECT_VERSION

release = PROJECT_VERSION

# 🎨 -- General configuration ---------------------------------------------------
# The general styling is set using the pydata-sphinx-theme (https://pydata-sphinx-theme.readthedocs.io/en/stable/index.html).
# On top of that, we override some of the CSS values, see property 'html_css_files'.
extensions = ["pydata_sphinx_theme"]

# needs_extensions = {
#     'pydata_sphinx_theme': '0.16',
# }

rst_epilog = f'''
.. |project| replace:: {project}
'''

nitpicky = True

# 🔡 -- Internationalization ----------------------------------------------------

# Specifying the natural language populates some key tags
language = "en"

# 🛠️ -- Builder options ---------------------------------------------------------
html_theme = "pydata_sphinx_theme"

html_theme_options = {
    # "external_links": [
    #     {
    #         "url": "https://iride-cyberitaly.space",
    #         "name": "Insula Perception",
    #     },
    # ],
    "header_links_before_dropdown": 6,
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/SercoSPA/cyberitaly-documentation",
            "icon": "fa-brands fa-github",
        }
    ],
    "logo": {
        # It supports HTML
        "text": PROJECT_NAME + ' - ' + PROJECT_VERSION,
        "image_dark": "_static/logo.png",
    },
    # "use_edit_page_button": True,
    "navbar_align": "content",
    "show_nav_level": 6,
    # "announcement": "https://raw.githubusercontent.com/pydata/pydata-sphinx-theme/main/docs/_templates/custom-template.html",
    "show_version_warning_banner": False,
    "navbar_center": ["navbar-nav"],
    "footer_start": ["copyright"],
    "footer_center": ["sphinx-version"],
    "back_to_top_button": True,
}

html_title = PROJECT_NAME

html_logo = "_static/logo.png"

html_favicon = '_static/favicon.ico'

# Add any paths that contain custom static files (such as style sheets) here.
# They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_css_files = [
    './_index.css'
]

html_static_path = ['_static']

html_permalinks_icon = "&#128279"

html_sourcelink_suffix = ""

# to reveal the build date in the pages meta, see html dom head.
html_last_updated_fmt = ""

html_sidebars = {}

html_context = {
    "default_mode": "dark",
}
