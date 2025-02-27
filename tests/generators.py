"""File containing generator functions to be used to compose testing strategies."""

import string

import hypothesis.strategies as st
import networkx as nx
import numpy as np
from hypothesis import assume
from hypothesis.extra.numpy import arrays
from more_itertools import unique

LETTERS = string.ascii_letters


@st.composite
def name_generator(
    draw: st.DrawFn,
    alphabet: str = LETTERS,
    min_size: int = 3,
    max_size: int = 5,
) -> str:
    """Generate a random name."""
    return draw(st.text(alphabet, min_size=min_size, max_size=max_size))


@st.composite
def names_generator(
    draw: st.DrawFn,
    alphabet: str = LETTERS,
    min_size: int = 3,
    max_size: int = 5,
) -> list[str]:
    """Generate a list of random names.

    Args:
    ----
        draw: hypothesis draw function
        alphabet: allowable characters
        min_size: miniumum size of names
        max_size: maximum size of names

    Returns
    -------
       list of random names.

    """
    return draw(
        st.lists(
            name_generator(alphabet),
            min_size=min_size,
            max_size=max_size,
            unique=True,
        )
    )


@st.composite
def subselection(
    draw: st.DrawFn, items: list, min_size: int = 1, max_size: int | None = None
) -> list:
    """Generate a random subselection from a list of unique items.

    Args:
    ----
        draw: hypothesis draw function
        items: from which to draw
        min_size: minimum number of items to draw
        max_size: (Optional) maximum number of items to draw.
                  If left empty, draws at most n-1 items.

    Returns
    -------
       list of unique random items subsampled from input items.

    """
    n_items = len(list(unique(items)))
    max_size = max(min_size, n_items - 1) if max_size is None else max_size
    assert min_size <= n_items, "Fewer items available than min_size."
    assert max_size <= n_items, "Fewer items available than max_size."
    return draw(
        st.lists(
            st.sampled_from(items),
            min_size=min_size,
            max_size=max_size,
            unique=True,
        )
    )


@st.composite
def dag_generator(
    draw: st.DrawFn, nodes: list[str], *, no_isolates: bool = False
) -> nx.DiGraph:
    """Generate a random DAG.

    Args:
    ----
        draw: hypothesis draw function
        nodes: names of nodes in the graph
        no_isolates: (Optional) whether isolated nodes are allowable.
                     Defaults to False.

    Returns
    -------
       Random nx.DiGraph that is a DAG.

    """
    # Any DAG has an adjacency matrix where the diagonal and upper triangle
    # are zeros. This means we can simply create a random nxn matrix and remove
    # the unwanted values.
    shape = len(nodes), len(nodes)
    adj_matrix = np.tril(
        draw(arrays(np.int32, shape, elements=st.integers(0, 1))), k=-1
    )
    graph = nx.from_numpy_array(  # pyright: ignore[reportCallIssue]
        adj_matrix, create_using=nx.DiGraph, nodelist=nodes
    )
    # sanity check whether this is a DAG
    assume(nx.is_directed_acyclic_graph(graph))
    if no_isolates:
        assume(nx.number_of_isolates(graph) == 0)
        assume(len(list(nx.weakly_connected_components(graph))) == 1)
    return graph
