from typing import Iterable, Optional, Sequence, Tuple
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text

console = Console()


def print_header(title: str, highlight_color: str = "cyan") -> None:
    console.print(Panel.fit(Text(title, style=f"bold {highlight_color}")))


def print_table(
    title: str,
    columns: Sequence[Tuple[str, Optional[str]]],
    rows: Iterable[Sequence[object]],
    highlight_color: str = "cyan",
) -> None:
    table = Table(title=title, title_style=f"bold {highlight_color}", show_lines=False)
    for col_title, justify in columns:
        table.add_column(col_title, justify=justify or "left", overflow="fold")
    for row in rows:
        table.add_row(*[str(cell) if cell is not None else "" for cell in row])
    console.print(table)


def print_info(message: str) -> None:
    console.print(f"[bold green]✓[/bold green] {message}")


def print_warn(message: str) -> None:
    console.print(f"[bold yellow]![/bold yellow] {message}")


def print_error(message: str) -> None:
    console.print(f"[bold red]✗[/bold red] {message}")

