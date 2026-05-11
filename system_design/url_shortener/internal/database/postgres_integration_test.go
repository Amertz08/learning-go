package database

import (
	"testing"

	"github.com/jackc/pgx/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.come/Amertz08/learning-go/system_design/url_shortener"
)

func TestPostgresIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "postgres integration")
}

var _ = Describe("postgres integration tests", func() {
	var pgStore *PGDataStore
	var conn *pgx.Conn
	var connErr error

	BeforeEach(func(ctx SpecContext) {
		conn, connErr = pgx.Connect(ctx, "postgres://postgres:password@localhost:5432/postgres")
		Expect(connErr).ToNot(HaveOccurred())
		row, connErr := conn.Query(ctx, url_shortener.SchemaSqlString)
		row.Close()
		Expect(connErr).ToNot(HaveOccurred())

		pgStore = NewPGDataStore(conn)
	})
	AfterEach(func(ctx SpecContext) {
		conn.QueryRow(ctx, url_shortener.DropSchemaSqlString)
		conn.Close(ctx)
	})
	When("creating a record", func() {
		It("works", func(ctx SpecContext) {
			// TODO
			_, err := pgStore.CreateShortenedRecord(ctx, "encoded", "target")
			Expect(err).To(BeNil())
		})
	})
})
