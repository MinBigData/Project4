
public class Driver {
	public static void main(String[] args) throws Exception {
		
		DataDividerByUser dataDividerByUser = new DataDividerByUser();
		CoOccurrenceMatrixGenerator coOccurrenceMatrixGenerator = new CoOccurrenceMatrixGenerator();
		DataDividerByMovie dataDividerByMovie = new DataDividerByMovie();
		Multiplication multiplication = new Multiplication();
		
		String[] path1 = {args[0], args[1]};
		String[] path2 = {args[1], args[2]};
		String[] path3 = {args[0], args[3]};
		String[] path4 = {args[5], args[3], args[4]};
		
		dataDividerByUser.main(path1);
		coOccurrenceMatrixGenerator.main(path2);
		dataDividerByMovie.main(path3);
		multiplication.main(path4);
		
	}

}
