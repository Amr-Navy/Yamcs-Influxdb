from xml.etree import ElementTree as ET

# Read the latest Yamcs versions from the Maven pom.xml
tree = ET.ElementTree()
tree.parse('../pom.xml')
version_el = tree.getroot().find('{http://maven.apache.org/POM/4.0.0}version')

project = u'yamcs-Influx'
copyright = u'2020, Space Applications Services'
author = u'Yamcs Team'

# The short X.Y version
version = version_el.text

# The full version, including alpha/beta/rc tags
release = version

extensions = [
    'sphinx.ext.extlinks',
    'sphinxcontrib.fulltoc',
]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = [u'_build', 'Thumbs.db', '.DS_Store']

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'

extlinks = {
    'yamcs-manual': ('https://docs.yamcs.org/yamcs-server-manual/%s', None),
}

latex_elements = {
    # The paper size ('letterpaper' or 'a4paper').
    #
    'papersize': 'a4paper',

    # The font size ('10pt', '11pt' or '12pt').
    #
    # 'pointsize': '10pt',

    # Additional stuff for the LaTeX preamble.
    #
    'preamble': '\setcounter{tocdepth}{2}',

    # Latex figure (float) alignment
    #
    'figure_align': 'htbp',
}

latex_documents = [
    ('index', 'yamcs-Influxdb-plugin.tex', 'Yamcs: Influxdb Plugin', 'Space Applications Services', 'howto'),
]

latex_show_urls = 'footnote'
