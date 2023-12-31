import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import re
from itertools import cycle

DF = pd.DataFrame

def stack(pivoted: DF) -> DF:
    df = pivoted.stack().reset_index(1)
    df.index = pd.to_datetime(df.index)
    df.columns = ['variable', 'value']
    return df


def cfg2subgraph(subplots: dict[list[dict]],
                 dfs: DF | dict[str, DF],
                 chart_type='Scatter',
                 var_col='variable',
                 val_col='value',
                 text_col: str = None,
                 customdata_cols: list = None,
                 override_func=lambda *_, **__: {},
                 height=600,
) -> go.Figure:
    ### setup ###
    dfs = dfs if isinstance(dfs, dict) else {'': dfs}
    keys = tuple(f'{dfkey} {cfgkey}' for cfgkey in subplots for dfkey in dfs)

    #### create subplot #####
    fig = make_subplots(len(keys),
        shared_xaxes=True,
        vertical_spacing=0.05,
        # row_heights=[CHART_SIZE] * len(subplots),
        subplot_titles=keys,
    )

    ### add traces ####
    subplot_row = 1
    for subplot in subplots.values():
        for dfkey, df in dfs.items():
            for cfg in subplot:
                chart_type = cfg.get('type', chart_type)
                kwargs_ = cfg.get('kwargs', {})
                kwargs_func = lambda *x: kwargs_ if isinstance(kwargs_, dict) else kwargs_(*x)
                for col, subdf in df.query(f"{var_col}.str.contains(@cfg['regex'])").groupby(var_col):
                    kwrgs = {
                        'hoverlabel': {'namelength': -1},
                        'name': col,
                        'customdata': subdf[customdata_cols] if customdata_cols else None,
                        'text': subdf[text_col] if text_col else None,
                        'meta': dict(col=col, dfkey=dfkey),
                    } | kwargs_func(col, dfkey) | override_func(col, dfkey)

                    gob = getattr(go, chart_type)(x=subdf.index, y=subdf[val_col], **kwrgs)
                    fig.add_trace(gob, row=subplot_row, col=1)
            subplot_row += 1

    # TODO: reorder legend into original input df order
    fig.update_xaxes(showticklabels=True)  # , tickangle=-45
    fig.update_layout(height=subplot_row * height, legend=dict(
        groupclick="toggleitem")
    )

    return fig

################################# CHART COLOURS #########################################



if __name__ == '__main__':
    from swarkn.charting.colors import color_cfg, cycle_colors
    from functools import partial
    import plotly.io as pio
    import pandas as pd
    pio.renderers.default = "browser"

    cfg = {
        'curves1': [
            {
                'regex': 'curves1_',
                'colors': partial(color_cfg, {}),
                'kwargs': {'line': {"dash": "dot"}},
            }, {
                'regex': 'curves1a_',
                #'type': 'Bar',
            },
        ],
        'curves2': [
            {
                'regex': 'curves2',
                'colors': cycle_colors,
                'kwargs': lambda col, dfkey, *_: dict(
                    line={"dash": "dot"},
                    hovertemplate='%{text} %{customdata}',
                    legendgroup='curves2',
                    legendgrouptitle_text='curve2_title',
                ),
            },
        ],
    }

    df = pd.DataFrame({
        'category': ['a'] * 5 + ['b'] * 5,
        'category_text': ['aa'] * 4 + ['bb'] * 6,
        "curves1_xx": list(range(10)),
        "curves1a_xx": list(range(10, 20)),
        "curves2_xx": list(range(10, 20)),
    }).reset_index().melt(['index', 'category', 'category_text']).set_index('index')
    fig = cfg2subgraph(cfg, df,
        # text_col='category_text',
        # customdata_cols='category',
    )

    fig.show()