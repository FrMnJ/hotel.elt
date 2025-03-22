import logging
from textual import on
from textual.app import App, ComposeResult
from textual.widgets import Header, Button, Input, Select, Log, Label
from textual.logging import TextualHandler
from textual.containers import Container
from output_save_options import OPTIONS

logging.basicConfig(
    level="NOTSET",
    handlers=[TextualHandler()],
)

class UIETL(App):
    CSS_PATH = "./styles/main.tcss"
    TITLE = "ETL Hotel"
    output_option = Select.BLANK
    is_sql = False
    def compose(self) -> ComposeResult:
        yield Header(id="header")
        yield Container(
            Label("Selecciona el formato de salida:"),
            Select(option for option in OPTIONS),
            Container(
                Label("Ruta completa del archivo de salida:", id="name-output-label"),
                Input(),
                id="name-output-container",
            ),
            id="form-container",
        )
        yield Container(Button("Ejecutar ETL", id="run", variant="primary"), id="button-container")
        yield Container(Log(auto_scroll=True, id="log"), id="log-container")
    
    def update_label(self) -> None:
        label = self.query_one("#name-output-label", Label)
        label.update("URI de conexión de bases de datos:" if self.is_sql else "Ruta completa del archivo de salida:")
    
    @on(Select.Changed)
    def select_changed(self, event: Select.Changed) -> None:
        self.output_option = event.value
        self.is_sql = self.output_option == "sql"
        self.update_label()
        if self.output_option is not Select.BLANK:
            self.query_one(Log).write_line(f"Opción seleccionada: {self.output_option}")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if self.output_option is Select.BLANK:
            self.query_one(Log).write_line("Selecciona un formato de salida")
        elif event.button.id == "run":
            self.query_one(Log).write_line("Ejecutando ETL...")

    def on_mount(self) -> None:
        self.query_one(Log).write_line("Inicializando la aplicacion...")
