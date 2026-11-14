package domain

import (
	"strings"
	"testing"
)

func TestRenderTemplateEscapes(t *testing.T) {
	sub, body, errs := RenderTemplate("template.subject", "template.body",
		`\[name\]`, `literal`, map[string]string{"name": "x"})
	if len(errs) != 0 {
		t.Fatalf("errs=%v", errs)
	}
	if sub != `[name]` || body != "literal" {
		t.Fatalf("sub=%q body=%q", sub, body)
	}
}

func TestRenderTemplateSubstituteOnce(t *testing.T) {
	sub, _, errs := RenderTemplate("template.subject", "template.body",
		"hi [name]", "", map[string]string{"name": "[evil]"})
	if len(errs) != 0 || sub != "hi [evil]" {
		t.Fatalf("sub=%q errs=%v", sub, errs)
	}
}

func TestRenderMissingVariable(t *testing.T) {
	_, _, errs := RenderTemplate("recipients[0].template.subject", "recipients[0].template.body",
		"[missing]", "", map[string]string{})
	if len(errs) != 1 || errs[0].Code != "missing_variable" {
		t.Fatalf("errs=%v", errs)
	}
}

func TestRenderEmptyVariableAllowed(t *testing.T) {
	sub, body, errs := RenderTemplate("template.subject", "template.body",
		"[name]", "x", map[string]string{"name": ""})
	if len(errs) != 0 {
		t.Fatalf("errs=%v", errs)
	}
	if sub != "" || body != "x" {
		t.Fatal(sub, body)
	}
}

func TestScanTemplateInvalidBracket(t *testing.T) {
	if errs := ScanTemplateSyntax("template.subject", "template.body", "a]b", ""); len(errs) == 0 {
		t.Fatal("expected invalid")
	}
}

func TestRenderStopsBeforeOversizedSubstitution(t *testing.T) {
	huge := strings.Repeat("x", MaxPayloadBytes+1)
	_, _, errs := RenderTemplate("recipients[0].template.body", "recipients[0].template.body",
		"[v]", "", map[string]string{"v": huge})
	if len(errs) != 1 || errs[0].Code != "too_large" {
		t.Fatalf("errs=%v", errs)
	}
}

func TestScanTemplateInvalidEscape(t *testing.T) {
	if errs := ScanTemplateSyntax("template.subject", "template.body", `\q`, ""); len(errs) == 0 || errs[0].Code != "invalid_escape" {
		t.Fatalf("errs=%v", errs)
	}
}
