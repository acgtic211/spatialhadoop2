package edu.umn.cs.spatialHadoop.operations.dendis;

public class Utils {
  public static double [][]Reservmemdouble(int n, int p)
  {
    int i;
    double [][]data;
    System.out.println(n+"x"+p);
    System.out.println((Runtime.getRuntime().totalMemory()/1024/1024)+":"+(Runtime.getRuntime().maxMemory()/1024/1024));

    data = new double[n][p];

    System.out.println((Runtime.getRuntime().totalMemory()/1024/1024)+":"+(Runtime.getRuntime().maxMemory()/1024/1024));

    return data;
  }

  public static void DisablePrototype(double []d_max,double []dist_proto,int N_proto,int []disable)
  {
    int i;

    for (i=0;i<N_proto - 1;i++)
    {
      if (d_max[i] < 0.5 * dist_proto[i])
        disable[i] = 1;
      else
        disable[i] = 0;
    }
  }

  public static int InegalTriangular(double [][]D_cluster,double D_nearest,int Cluster_actuel, int Cluster_candidat)
  {
    if (D_cluster[Cluster_actuel][Cluster_candidat] >= 2*D_nearest)
      return 0;
    else
      return 1;
  }

  public static void CalculusStatVectorBool(double[] pattern, int N_pattern, int[] booleen, Double Moy, Double Ecart, Double Min, Double Max) {
    int i;
    int Occurence = 0;

    Ecart = 0.;
    Moy = 0.;
    Min = 10000.;
    Max = -10000.;


    for (i=0;i<N_pattern;i++)
      if (booleen[i] == 1)
      {
        Moy = Moy + pattern[i];

        if (pattern[i] < Min)
          Min = pattern[i];
        if (pattern[i] > Max)
          Max = pattern[i];

        Occurence++;
      }

    if (Occurence != 0)
      Moy = Moy / (double)(Occurence);
    else
      Moy = 0.;

    for (i=0;i<N_pattern;i++)
      if (booleen[i] == 1)
        Ecart = Ecart + (Moy - pattern[i]) * (Moy - pattern[i]);

    if (Occurence != 0)
      Ecart = Math.sqrt(Ecart / (double)(Occurence));
    else
      Ecart = 0.;
  }
  
  public static void SortPrototypes_DENDIS(Internal I,ParamFCNNrule P, Integer N_instance, int [][]Liste_pattern, int Min)
  {
    int i,j;
    int []Weight_tempon;
    int [][]Liste_tempon;
    int []SelectionIndividu;
    int compteur = 0;

    if (P.Auto == 0)
      return;

    SelectionIndividu = new int[N_instance];
    Liste_tempon = new int[N_instance][];
    Weight_tempon = new int[N_instance];


    compteur = 0;
    for (i=0;i<N_instance;i++)
    {
      Liste_tempon[i] = null;
      if (I.Weight[i]>0)
      {
        Weight_tempon[compteur] = I.Weight[i];
        Liste_tempon[compteur] = new int[I.Weight[i]];
        for (j=0;j<I.Weight[i];j++)
          Liste_tempon[compteur][j] = Liste_pattern[i][j];//MEMOIRE!!
        SelectionIndividu[compteur] = I.Liste_selectionne[i];
        compteur++;
      }
      if (Liste_pattern[i]!=null)
      {
        Liste_pattern[i] = null;
      }
    }

    for (i=0;i<compteur;i++)
    {
      Liste_pattern[i] = new int[Weight_tempon[i]];

      for (j=0;j<Weight_tempon[i];j++)
        Liste_pattern[i][j] = Liste_tempon[i][j];
      I.Liste_selectionne[i] = SelectionIndividu[i];
      I.Weight[i]=Weight_tempon[i];
    }

    N_instance = compteur;
  }
  
  public static int UpdateWithGravity_DENDIS(double [][]data, Internal I,ParamFCNNrule P, int N_instance)
  {
    int i,j,winner;
    int []I_mod;
    int [][]liste;
    int []masse;
    Integer pattern_actif;
    double dist_min, dist_calc;

    I_mod = new int[N_instance];
    liste = new int[N_instance][];
    masse = new int[N_instance];

    P.pattern = new double[N_instance][P.p];

    for (i=0;i<N_instance;i++)//pour tous les �l�ments de T.
    {
      liste[i] = null;
      masse[i] = 0;
      if (I.Pre_distance[i] > P.S2_Beta * P.dav_1knn[N_instance - 2])
        I_mod[i] = 1;
      else
        I_mod[i] = 0;

      for (j=0;j<P.p;j++)
        P.pattern[i][j] = 0;
    }

    if (P.S2_Option_all == 1)
    {
      for (i=0;i<P.n;i++)//pour tous les �l�ments de T.
      {
        winner = I.Nearest_proto[i];//l'instance la plus proche est m�moris�...on garde en m�moire l'ancien.
        for (j=0;j<P.p;j++)
          P.pattern[winner][j] = P.pattern[winner][j] + data[i][j];
      }

      for (i=0;i<N_instance;i++)
        for (j=0;j<P.p;j++)
          P.pattern[i][j] /= (double)(I.Weight[i]);
    }
    else // on ne regarde que ceux qui sont exentr�s.
    {
      for (i=0;i<P.n;i++)//pour tous les �l�ments de T.
      {
        winner = I.Nearest_proto[i];//l'instance la plus proche est m�moris�...on garde en m�moire l'ancien.
        if (I_mod[winner] == 1)
        {
          if (liste[winner] == null)
            liste[winner] = new int[I.Weight[winner]];

          liste[winner][masse[winner]] = i;
          masse[winner] += 1;

          for (j=0;j<P.p;j++)
            P.pattern[winner][j] = P.pattern[winner][j] + data[i][j];
        }
      }

      //on calcule le barycentre pour les instances concern�s.
      for (i=0;i<N_instance;i++)
        if (I_mod[i] == 1)
        {
          //le nouveau centre.
          for (j=0;j<P.p;j++)
            P.pattern[i][j] /= (double)(I.Weight[i]);
          dist_min = 1000;
          for (j=0;j<masse[i];j++)//pour les patterns de ce prototype.
          {
            pattern_actif = liste[i][j];//le pattern.
            dist_calc = P.fdist(data[pattern_actif],P.pattern[i],P.p);
            if (dist_calc < dist_min)
            {
              dist_min = dist_calc;
              I.Liste_selectionne[i] = pattern_actif;
            }
          }
        }
    }

    //on calcule la moyenne des voisinages.


    //On �limine le BRUIT.
    for (i=0;i<N_instance;i++)
    {
      if (I.Weight[i] <= 0.1*P.Granularity*(double)P.n)
        I.Weight[i] = 0;

      if (I.Weight[i] <= (0.5*P.Granularity*(double)P.n) && I.Pre_distance[i] >= P.S2_Beta*P.dav_1knn[N_instance - 2])
        I.Weight[i] = 0;
    }

    P.pattern = null;

    return 1;
  }
}
